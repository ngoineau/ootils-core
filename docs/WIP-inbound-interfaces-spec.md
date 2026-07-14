# WIP — Spécifications interfaces entrantes

> **⚠️ SUPERSEDED/ARCHIVÉ — 2026-07-13.** Ce brouillon V0 (catalogue théorique M01-S04, jamais implémenté) est remplacé comme plan actif par [`ADR-042`](ADR-042-interface-doctrine.md) et sa face pilote [`DOCTRINE-INTERFACES.md`](DOCTRINE-INTERFACES.md), qui posent la doctrine V1 réellement décidée par le pilote (pivot fichier TSV, 4-5 flux gouvernés, pas 30+ interfaces catalogue). Le catalogue ci-dessous reste une référence de nommage pour d'éventuelles phases V2/V3, archivé tel quel, non réécrit.

**Date** : 2026-05-25
**Statut** : Brouillon V0 pour décantation.
**Cadrage** : `CLAUDE.md` § North Star + `docs/AGENT-FLEET-CATALOG.md` §5bis (Pilotage interfaces).

> Toute interface entrante est un **contrat versionné** consommé sous SLA par
> Ootils. Le pilotage agent (Interface Health Watcher, Schema Drift Watcher,
> Mapping Drift Watcher, Replay Agent, Contract Compliance Gate) repose sur ces
> spécifications. Sans contrat formel, pas d'agent fiable.

---

## 0. Principes communs à toutes les interfaces entrantes

### 0.1 Contrat versionné

Chaque interface est identifiée par `(source_system, entity, contract_version)`.
Exemple : `(ERP_SAP, purchase_orders, v1.2)`.

- `contract_version` SemVer. Breaking change → nouvelle version, ancienne reste servie pendant grace period.
- Schéma publié dans `docs/contracts/<source>/<entity>/<version>.json` (JSON Schema).
- Validation au ingress, rejet ligne par ligne si non conforme.

### 0.2 Cadence et SLA

| Profil | Fréquence | SLA freshness | Mode |
|---|---|---|---|
| Real-time | événement | < 60 s | push (webhook / Kafka / gRPC) |
| Near-real-time | minute | < 5 min | poll API / streaming |
| Frequent | 15 min | < 30 min | poll API |
| Periodic | horaire | < 2 h | batch SFTP / API |
| Daily | nuit | < 24 h | full snapshot CSV / API |
| On-demand | manuel | – | upload UI / API |

SLA stocké dans table `import_contracts` (à créer). Interface Health Watcher + Import Freshness Gate consomment ces SLA.

### 0.3 Idempotency

- Toute ligne porte un `source_ref` unique par `(source_system, entity)`.
- Re-ingestion de même `source_ref` avec payload identique = no-op.
- Re-ingestion de même `source_ref` avec payload différent → **politique configurée par contrat** :
  - `REJECT` (défaut master data critique)
  - `REPLACE_LAST_WINS` (défaut transactionnel)
  - `VERSION_HISTORY` (audit-critique : conserve toutes les versions)

### 0.4 Granularité

- **SNAPSHOT** : état complet à un instant T (typique on-hand inventory). Remplace tout.
- **DELTA** : changements depuis dernier batch (typique master data). Upsert.
- **EVENT** : événement métier individuel (typique sales order, shipment). Append.
- **CDC** : Change Data Capture (insert/update/delete). Patch.

### 0.5 Structure unifiée d'un batch d'ingestion

```
ingest_batch:
  batch_id:           UUID
  source_system:      TEXT
  entity:             TEXT
  contract_version:   TEXT
  granularity:        SNAPSHOT|DELTA|EVENT|CDC
  received_at:        TIMESTAMPTZ
  source_window:      [from, to]            -- pour les batchs sur fenêtre temporelle
  row_count:          INT
  status:             RECEIVED|VALIDATING|ACCEPTED|REJECTED|PARTIAL|REPLAYING
  ...                                       -- déjà partiellement en place via migration 023, 033
```

### 0.6 Pipeline ingress standard

```
1. Receive      → écriture brute table `staging.raw_<entity>` (audit immuable)
2. Validate     → JSON Schema + règles métier → erreurs ligne par ligne
3. DQ gate      → Data Quality Watcher signale, Contract Compliance Gate accepte/refuse
4. Map          → codes externes → UUID internes (Mapping Drift Watcher)
5. Approve      → automatique si DQ OK, manuel sinon (staging.approve)
6. Apply        → upsert/append dans tables business
7. Emit         → change events sur StreamChanges
8. Propagate    → engine déclenche dirty subgraph (si entité graphe)
9. Audit        → ledger complet (qui, quand, quoi, quel résultat)
```

L'infra `staging/` (parser, loader, diff, approve, reject) + migrations 023, 033, 035, 036 couvrent déjà partiellement le pipeline. À reconvoluer en contrat-first.

### 0.7 Emit change events vers agents

Chaque ingestion réussie émet **au moins** :
- `import.batch.completed` (toujours, avec metrics)
- `import.batch.failed` (si rejet)
- `<entity>.upserted` / `<entity>.created` / `<entity>.deleted` (par ligne ou agrégé)
- `propagation.triggered` (si entité graphe)

Topics consommés par Watchers / Scenario agents / Orchestrator.

---

## 1. Catalogue des interfaces entrantes

### Master data

| # | Entité | Sources typiques | Granularité | Cadence | Phase | Watcher dédié |
|---|---|---|---|---|---|---|
| **M01** | Items / SKUs | ERP, PLM | DELTA | Daily | V1 | DQ + Schema Drift |
| **M02** | Locations / Sites | ERP, WMS | DELTA | Daily | V1 | DQ |
| **M03** | Customers | CRM, ERP | DELTA | Daily | V1 | DQ |
| **M04** | Suppliers | ERP | DELTA | Daily | V1 | DQ |
| **M05** | BOM (Bill of Materials) | ERP, PLM | DELTA | Daily | V1 | BOM Health |
| **M06** | Routings / Operations | ERP, MES | DELTA | Daily | V2 | DQ |
| **M07** | Resources / Work Centers | ERP, MES | DELTA | Daily | V2 | Capacity |
| **M08** | Calendars (working / shutdown) | ERP, HR | DELTA | Weekly | V1 | Calendar Coherence |
| **M09** | Sourcing rules / Lanes | ERP, TMS | DELTA | Weekly | V2 | DQ |
| **M10** | Unit of Measure conversions | ERP | DELTA | On-change | V1 | DQ |
| **M11** | Currencies / FX rates | ERP, Finance | SNAPSHOT | Daily | V3 | DQ |

### Transactional

| # | Entité | Sources | Granularité | Cadence | Phase | Watcher |
|---|---|---|---|---|---|---|
| **T01** | On-hand inventory | WMS, ERP | SNAPSHOT | Near-real-time | V1 | Inventory + Import Health |
| **T02** | Purchase Orders | ERP | DELTA / EVENT | Near-real-time | V1 | Supply + Lead Time Drift |
| **T03** | Work Orders | ERP, MES | DELTA / EVENT | Near-real-time | V2 | Capacity |
| **T04** | Transfer Orders | ERP, WMS | DELTA / EVENT | Near-real-time | V2 | Inventory |
| **T05** | Inventory Adjustments | WMS, ERP | EVENT | Near-real-time | V2 | DQ + Inventory |
| **T06** | Goods Receipts (PO receipts) | WMS | EVENT | Real-time | V1 | Supply + Lead Time Drift |
| **T07** | Goods Issues (consumption) | WMS, MES | EVENT | Real-time | V2 | Inventory |
| **T08** | Production Confirmations | MES, ERP | EVENT | Real-time | V2 | Capacity |

### Demand

| # | Entité | Sources | Granularité | Cadence | Phase | Watcher |
|---|---|---|---|---|---|---|
| **D01** | Customer Orders | ERP, e-com | EVENT / DELTA | Real-time | V1 | Service Risk + Demand Anomaly |
| **D02** | Shipments (sortie effective) | WMS, TMS | EVENT | Real-time | V1 | Demand Anomaly (alimente `demand_history`) |
| **D03** | POS / Sell-out | Retail, e-com | EVENT | Frequent | V2 | Demand Anomaly |
| **D04** | External forecasts (sales, marketing) | Excel, BI, customer EDI | DELTA | Weekly | V2 | Forecast Accuracy |
| **D05** | Promotions / Events | Marketing, CRM | EVENT | On-change | V3 | Promo / Event |
| **D06** | New Product Introductions | PLM, marketing | EVENT | On-change | V3 | – |

### Paramètres planning

| # | Entité | Sources | Granularité | Cadence | Phase | Watcher |
|---|---|---|---|---|---|---|
| **P01** | Safety Stock | ERP, Excel planners | DELTA | On-change | V1 | Safety Stock Adequacy + Parameter Change Audit |
| **P02** | MOQ / Lot sizing | ERP | DELTA | On-change | V2 | MOQ/Lot Size + Parameter Change Audit |
| **P03** | Lead Times | ERP | DELTA | On-change | V1 | Lead Time Drift + Parameter Change Audit |
| **P04** | Reorder Points | ERP, Excel | DELTA | On-change | V2 | Reorder Point |
| **P05** | ABC / XYZ classification | BI, Excel | DELTA | Weekly | V2 | ABC/XYZ Drift |
| **P06** | Coverage targets | Excel, planners | DELTA | On-change | V3 | Coverage Target |
| **P07** | Allocation rules | ERP, commercial | DELTA | On-change | V3 | Allocation Rule |

### Supplier-side (collaboration)

| # | Entité | Sources | Granularité | Cadence | Phase | Watcher |
|---|---|---|---|---|---|---|
| **S01** | PO Confirmations (supplier ack) | Supplier portal, EDI 855 | EVENT | Real-time | V2 | Supply |
| **S02** | ASN (Advance Ship Notice) | Supplier portal, EDI 856 | EVENT | Real-time | V2 | Supply |
| **S03** | Supplier capacity declarations | Supplier portal | DELTA | Weekly | V3 | Supplier |
| **S04** | Supplier OTIF feedback | Supplier portal | EVENT | Monthly | V3 | Supplier |

---

## 2. Format de spécification d'une interface entrante

Chaque interface est documentée dans `docs/contracts/<source>/<entity>/<version>.md` avec ce gabarit :

```yaml
contract:
  source_system:      STRING       # ERP_SAP | WMS_X | EDI_855 | MANUAL_CSV | API_CLIENT
  entity:             STRING       # purchase_orders | items | shipments | ...
  version:            SEMVER       # 1.0.0
  status:             DRAFT|ACTIVE|DEPRECATED
  effective_from:     DATE
  deprecated_at:      DATE?

transport:
  mode:               PUSH_API|POLL_API|SFTP|S3|KAFKA|WEBHOOK|UI_UPLOAD
  endpoint:           STRING       # ex: POST /v1/ingest/purchase_orders
  auth:               BEARER|MTLS|OAUTH2|API_KEY
  payload_format:     JSON|CSV|XML|EDI|PARQUET

cadence:
  profile:            REAL_TIME|NEAR_REAL_TIME|FREQUENT|PERIODIC|DAILY|ON_DEMAND
  sla_freshness_s:    INT          # seuil agent freshness gate
  expected_size:      {p50, p95, max}

granularity:          SNAPSHOT|DELTA|EVENT|CDC

idempotency:
  source_ref_field:   STRING       # nom du champ business unique
  conflict_policy:    REJECT|REPLACE_LAST_WINS|VERSION_HISTORY

schema:
  required_fields:    [...]
  optional_fields:    [...]
  dimensions:         [channel, region, customer_segment, ...]    # pour demand
  ref_to:             [items, locations, ...]                     # joins master data

validation:
  json_schema:        PATH
  business_rules:     [...]        # ex: lead_time_days > 0, quantity > 0

dq_checks:
  ingress:            [...]        # règles bloquantes à l'ingress
  post_load:          [...]        # règles non-bloquantes mais loguées
  agent_owners:       [DQ_Watcher, BOM_Health_Watcher, ...]

mapping:
  external_codes:     {item_code → item_id, location_code → location_id}
  fallback_policy:    REJECT|QUARANTINE|AUTO_CREATE

emit:
  topics:             [import.batch.completed, items.upserted, ...]
  propagate_dirty:    BOOL         # déclenche propagation engine si entité graphe

audit:
  retention_days:     INT          # raw payload retention
  pii_fields:         [...]        # champs à masquer dans logs
```

---

## 3. Spécifications détaillées — interfaces V1 (wedge shortage)

Les 11 interfaces V1 nécessaires au wedge :

| Code | Entité | Doc à produire |
|---|---|---|
| M01 | Items | `docs/contracts/items/v1.md` |
| M02 | Locations | `docs/contracts/locations/v1.md` |
| M03 | Customers | `docs/contracts/customers/v1.md` |
| M04 | Suppliers | `docs/contracts/suppliers/v1.md` |
| M05 | BOM | `docs/contracts/bom/v1.md` |
| M08 | Calendars | `docs/contracts/calendars/v1.md` |
| M10 | UoM | `docs/contracts/uom/v1.md` |
| T01 | On-hand inventory | `docs/contracts/on_hand/v1.md` |
| T02 | Purchase Orders | `docs/contracts/purchase_orders/v1.md` |
| T06 | Goods Receipts | `docs/contracts/goods_receipts/v1.md` |
| D01 | Customer Orders | `docs/contracts/customer_orders/v1.md` |
| D02 | Shipments | `docs/contracts/shipments/v1.md` |
| P01 | Safety Stock | `docs/contracts/safety_stock/v1.md` |
| P03 | Lead Times | `docs/contracts/lead_times/v1.md` |

(14 contrats à formaliser pour le wedge V1.)

### 3.1 Exemple détaillé — T02 Purchase Orders v1

```yaml
contract:
  source_system:      ERP_GENERIC
  entity:             purchase_orders
  version:            1.0.0
  status:             DRAFT

transport:
  mode:               PUSH_API
  endpoint:           POST /v1/ingest/purchase_orders
  auth:               BEARER
  payload_format:     JSON

cadence:
  profile:            NEAR_REAL_TIME
  sla_freshness_s:    300                # 5 min
  expected_size:      {p50: 50, p95: 500, max: 10000}

granularity:          DELTA              # upsert sur source_ref

idempotency:
  source_ref_field:   po_number + line_number
  conflict_policy:    REPLACE_LAST_WINS

schema:
  required_fields:
    - po_number          # STRING
    - line_number        # INT
    - supplier_code      # STRING → mapping → supplier_id
    - item_code          # STRING → mapping → item_id
    - delivery_location_code   # STRING → mapping → location_id
    - quantity           # NUMERIC(18,4) > 0
    - uom                # STRING (validé via UoM)
    - promised_date      # DATE
    - status             # ENUM(OPEN, CONFIRMED, IN_TRANSIT, RECEIVED, CANCELLED)
  optional_fields:
    - order_date         # DATE
    - confirmation_date  # DATE
    - supplier_ack_date  # DATE
    - unit_cost          # NUMERIC
    - currency           # STRING (3-letter)
    - incoterm           # STRING
    - tracking_number    # STRING
    - notes              # TEXT (PII-scanned)

validation:
  json_schema:        docs/contracts/purchase_orders/v1.schema.json
  business_rules:
    - promised_date >= order_date
    - quantity > 0
    - if status = RECEIVED then confirmation_date is not null

dq_checks:
  ingress:
    - supplier_code resolves to active supplier
    - item_code resolves to active item
    - delivery_location_code resolves to active location
  post_load:
    - lead_time_observed = promised_date - order_date is within [supplier.lead_time_min, supplier.lead_time_max*2]
        # alerte Lead Time Drift Watcher
  agent_owners:       [DQ_Watcher, Supply_Watcher, Lead_Time_Drift_Watcher]

mapping:
  fallback_policy:    QUARANTINE         # PO avec supplier inconnu → quarantine + Mapping Repair Agent

emit:
  topics:
    - import.batch.completed
    - purchase_orders.upserted
    - propagation.triggered
  propagate_dirty:    TRUE               # PO change → impacte ProjectedInventory

audit:
  retention_days:     365
  pii_fields:         [notes]
```

### 3.2 Exemple détaillé — D02 Shipments v1 (alimente `demand_history`)

```yaml
contract:
  source_system:      WMS_GENERIC
  entity:             shipments
  version:            1.0.0
  status:             DRAFT

transport:
  mode:               PUSH_API
  endpoint:           POST /v1/ingest/shipments
  payload_format:     JSON

cadence:
  profile:            REAL_TIME
  sla_freshness_s:    60

granularity:          EVENT              # append-only

idempotency:
  source_ref_field:   shipment_id + line_number
  conflict_policy:    REJECT             # un shipment ne se réécrit pas

schema:
  required_fields:
    - shipment_id        # STRING
    - line_number        # INT
    - item_code          # → item_id
    - location_code      # → location_id (origine)
    - quantity           # > 0
    - uom
    - shipped_at         # TIMESTAMPTZ
    - customer_code      # → customer_id
    - order_ref          # référence customer_order (D01)
  optional_fields:
    - channel            # B2B|B2C|E-COM|RETAIL
    - region             # ISO ou code interne
    - customer_segment   # KEY_ACCOUNT|SMB|...
    - order_type         # STANDARD|PROMO|PROJECT|SAMPLE
    - carrier
    - tracking
    - attrs              # JSONB pour dimensions extensibles

emit:
  topics:
    - import.batch.completed
    - shipments.appended
    - demand_history.appended              # déclenche Demand Anomaly Watcher
    - propagation.triggered                # baisse OnHand
  propagate_dirty:    TRUE

audit:
  retention_days:     2555                 # 7 ans (audit fiscal)
```

Note : `shipments` est **la source primaire de `demand_history`** (cf. WIP Demand §D1 recommandé : filtre `event_type IN ('SHIPMENT')`).

### 3.3 Exemple détaillé — P03 Lead Times v1

```yaml
contract:
  source_system:      ERP_GENERIC
  entity:             lead_times
  version:            1.0.0

transport:
  mode:               PUSH_API
  endpoint:           POST /v1/ingest/lead_times

cadence:
  profile:            ON_DEMAND           # changement manuel ou ERP push
  sla_freshness_s:    86400               # 24h

granularity:          DELTA

idempotency:
  source_ref_field:   item_code + supplier_code + location_code
  conflict_policy:    VERSION_HISTORY     # audit obligatoire sur paramètres

schema:
  required_fields:
    - item_code
    - supplier_code
    - location_code
    - lead_time_days     # INT > 0
    - effective_from     # DATE
  optional_fields:
    - lead_time_min_days
    - lead_time_max_days
    - lead_time_stddev_days
    - source             # 'CONTRACT' | 'OBSERVED' | 'PLANNER_OVERRIDE'
    - effective_to       # DATE

validation:
  business_rules:
    - lead_time_days > 0
    - lead_time_min_days <= lead_time_days <= lead_time_max_days

dq_checks:
  post_load:
    - alerte Parameter Change Audit Watcher (qui a poussé ce changement)
    - alerte Lead Time Drift Watcher si écart > 20% vs lead_time_observed

emit:
  topics:
    - import.batch.completed
    - parameters.lead_times.upserted
    - parameter.changed                   # déclenche Parameter Coherence Watcher
  propagate_dirty:    TRUE                # impacte tous les PI downstream
```

### 3.4 Autres interfaces V1 — à formaliser sur le même modèle

M01 Items, M02 Locations, M03 Customers, M04 Suppliers, M05 BOM, M08 Calendars,
M10 UoM, T01 On-hand, T06 Goods Receipts, D01 Customer Orders, P01 Safety Stock
→ chacune un fichier `docs/contracts/<entity>/v1.md` avec le même gabarit.

---

## 4. Endpoints REST à exposer pour V1

Pattern unifié sous `/v1/ingest/<entity>` :

```
POST /v1/ingest/items                  # M01
POST /v1/ingest/locations              # M02
POST /v1/ingest/customers              # M03
POST /v1/ingest/suppliers              # M04
POST /v1/ingest/bom                    # M05
POST /v1/ingest/calendars              # M08
POST /v1/ingest/uom                    # M10
POST /v1/ingest/on_hand                # T01
POST /v1/ingest/purchase_orders        # T02
POST /v1/ingest/goods_receipts         # T06
POST /v1/ingest/customer_orders        # D01
POST /v1/ingest/shipments              # D02
POST /v1/ingest/safety_stock           # P01
POST /v1/ingest/lead_times             # P03
```

Réponse standard :

```json
{
  "batch_id": "uuid",
  "status": "ACCEPTED|PARTIAL|REJECTED",
  "received": 1234,
  "accepted": 1230,
  "rejected": 4,
  "rejection_details": [...],
  "emitted_events": ["purchase_orders.upserted", "propagation.triggered"]
}
```

Sécurité : bearer token (OOTILS_API_TOKEN existant) + per-source scope (à ajouter).
Body cap : 10 MB (déjà en place via `IngestPayloadSizeLimitMiddleware`).
Idempotency-Key header obligatoire (cf. migration 023).

---

## 5. Cross-cutting — points à clarifier

| # | Sujet | Question |
|---|---|---|
| **I1** | Bus événements | NATS / Redis Streams / PG LISTEN-NOTIFY / Kafka pour `import.batch.completed` etc. ? |
| **I2** | Schema registry | Stocker JSON Schemas dans git (`docs/contracts/`) ou dans une table PG `contract_registry` ? |
| **I3** | Mapping | Table dédiée `external_code_mapping` (source, entity, external_code, internal_id) ? Existe-t-elle déjà ? |
| **I4** | Quarantine | Lignes rejetées : combien de temps ? UI pour les corriger ? Auto-purge après N jours ? |
| **I5** | Replay | Mécanisme pour rejouer un batch après correction (Replay Agent) — versionner les batchs ? Permettre `POST /v1/ingest/batches/{id}/replay` ? |
| **I6** | Backfill | Reload massif historique pour `demand_history` initial — endpoint dédié `/v1/ingest/bulk/<entity>` avec mode `BACKFILL` ? |
| **I7** | Versioning contrat | Plusieurs versions actives simultanément ? Header `X-Contract-Version: 1.0` requis sur appel ? |
| **I8** | Multi-tenant | `tenant_id` au niveau du token ou du payload ? |
| **I9** | EDI / fichiers plats | Adaptateur EDI 850/855/856 → JSON canonical ? Phase V2 ou plus tard ? |
| **I10** | Webhooks sortants | Notification client externe quand batch refusé / DQ critique → V2 ? |

---

## 6. Liens avec l'existant

- Migration **023** : `api_request_audit` + `ingest_idempotency` → fondations en place
- Migration **033** : `staging_schema` → pipeline staging déjà partiellement en place
- Migration **035, 036** : `ingest_batches_planning_params`, `ingest_batches_routings` → patterns à généraliser
- Modules `staging/{parser,loader,diff,approve,reject}.py` → infra réutilisable
- Routers `api/routers/{ingest,staging}.py` → point d'entrée existant

**Ce qui manque (à créer)** :
- Table `import_contracts` (registry source × entity × version × SLA × policy)
- Table `external_code_mapping` (source, entity, external_code, internal_id, status)
- Table `import_quarantine` (lignes rejetées en attente correction)
- Middleware version contract (`X-Contract-Version` header)
- Module `staging/contracts.py` (chargement + validation contrat)
- Bus événements interne (choix I1)
- Endpoints `/v1/ingest/<entity>` manquants (10 sur 14)

---

## 7. Pour la reprise

1. Valider le catalogue §1 (manque-t-il une entité ?)
2. Trancher I1-I10
3. Décider l'ordre de formalisation des 14 contrats V1 (probablement M01-M05 + T01 + D01-D02 + P01 + P03 d'abord — la mécanique core)
4. Décider si on documente chaque contrat dans `docs/contracts/` (recommandé) ou inline dans le code
5. Une fois validé : créer les tasks d'implémentation (1 par contrat + 1 par cross-cutting)

Aucune ligne de code écrite, comme demandé.
