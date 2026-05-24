# Staging templates — file format contracts per entity

These templates are the **canonical contract** between Ootils and any external tool that pushes data through the staging pipeline (ADR-013).

Each entity supported by `ingest_batches.entity_type` has:

- A specification document (`<entity>.md`) listing required + optional columns, types, value constraints, and how they map to canonical tables
- An example file (`<entity>.tsv`) ready to feed into `POST /v1/staging/upload`

## Conventions across all templates

- **First line is the header** — exact column names from the spec (case-insensitive, leading/trailing spaces tolerated)
- **Encoding** : UTF-8 sans BOM (CP-1252 toléré en fallback avec warning DQ)
- **Séparateur** : TSV (`\t`) recommandé, CSV (`,` ou `;`) toléré avec auto-détection
- **Valeurs manquantes** : cellule vide pour optionnel, **jamais "NULL" ou "N/A"** (ces strings seraient stockées telles quelles)
- **Dates** : ISO 8601 (`YYYY-MM-DD`)
- **Décimales** : point `.` comme séparateur (jamais virgule), pas de séparateur de milliers
- **Booléens** : `true` / `false` (lowercase), `1` / `0` toléré

## Catalogue par entité

| Entité | Template | Refresh | Endpoint | Notes |
|--------|----------|---------|----------|-------|
| items | [items.md](items.md) — [items.tsv](items.tsv) | full reload | staging + `/v1/ingest/items` | Master, source SAP/ERP typique |
| locations | [locations.md](locations.md) — [locations.tsv](locations.tsv) | full reload | staging + `/v1/ingest/locations` | Master, faible volume |
| suppliers | [suppliers.md](suppliers.md) — [suppliers.tsv](suppliers.tsv) | full reload | staging + `/v1/ingest/suppliers` | Master, faible volume |
| supplier_items | [supplier_items.md](supplier_items.md) — [supplier_items.tsv](supplier_items.tsv) | full reload | staging + `/v1/ingest/supplier-items` | Master + commercial |
| planning_params | [planning_params.md](planning_params.md) | **SCD2 transparent** | `/v1/ingest/planning-params` | Versionné par effective_from/to invisible côté client. Voir [ADR-014 D3](../ADR-014-resources-units-scd2.md). |
| on_hand | [on_hand.md](on_hand.md) — [on_hand.tsv](on_hand.tsv) | full reload | staging + `/v1/ingest/on-hand` | Snapshot WMS, quotidien |
| purchase_orders | [purchase_orders.md](purchase_orders.md) — [purchase_orders.tsv](purchase_orders.tsv) | full reload | staging + `/v1/ingest/purchase-orders` | ERP, refresh quotidien |
| work_orders | [work_orders.md](work_orders.md) — [work_orders.tsv](work_orders.tsv) | full reload | staging + `/v1/ingest/work-orders` | MES, refresh quotidien |
| customer_orders | [customer_orders.md](customer_orders.md) — [customer_orders.tsv](customer_orders.tsv) | full reload | staging + `/v1/ingest/customer-orders` | ERP commercial |
| transfers | [transfers.md](transfers.md) — [transfers.tsv](transfers.tsv) | full reload | staging + `/v1/ingest/transfers` | Inter-locations |
| forecasts | [forecasts.md](forecasts.md) — [forecasts.tsv](forecasts.tsv) | full reload | staging + `/v1/ingest/forecast-demand` | Outil prévision |
| resources | [resources.md](resources.md) | upsert par external_id | `/v1/ingest/resources` | Ressources capacitaires (machines, lignes, équipes, work centers). Voir [ADR-014 D1+D2](../ADR-014-resources-units-scd2.md). |
| routings | [routings.md](routings.md) | full-reload par (item, sequence) | `/v1/ingest/routings` | Gammes de fabrication. Cohérence d'unités op ↔ resource (ADR-014 D2). |
| bom | [bom.md](bom.md) | replace BOM active | `/v1/ingest/bom` | Nomenclature 1-parent → N-composants. LLC recalculé. |
| calendars | [calendars.md](calendars.md) | upsert par (location, date) | `/v1/calendars/import` | Calendriers opérationnels par site. Fallback Mon-Fri si absent. |

## Modes d'ingestion

- **staging** : pipeline ADR-013 — upload fichier (TSV/CSV/XLSX/JSON) → DQ L1-L4 → diff → approve. Recommandé pour les volumes importants et les flux audités.
- **REST direct** : `POST /v1/ingest/*` ou endpoint dédié — payload JSON, plus simple côté intégrateur quand le client a déjà la donnée en mémoire (API → API).

Les deux modes finissent en table canonique. Les **règles DQ s'appliquent dans les deux cas**.

## Entités sans pipeline staging dédié

`resources`, `routings`, `bom`, `calendars` n'ont pas de chemin staging upload — uniquement REST direct. Leurs templates `.md` documentent le contrat JSON.
