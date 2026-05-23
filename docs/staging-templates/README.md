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

| Entité | Template | Refresh | Notes |
|--------|----------|---------|-------|
| items | [items.md](items.md) — [items.tsv](items.tsv) | full reload | Master, source SAP/ERP typique |
| locations | [locations.md](locations.md) — [locations.tsv](locations.tsv) | full reload | Master, faible volume |
| suppliers | [suppliers.md](suppliers.md) — [suppliers.tsv](suppliers.tsv) | full reload | Master, faible volume |
| supplier_items | [supplier_items.md](supplier_items.md) — [supplier_items.tsv](supplier_items.tsv) | full reload | Master + commercial |
| item_planning_params | *(à venir)* | **SCD2** | Versionné par effective_from/to |
| on_hand | [on_hand.md](on_hand.md) — [on_hand.tsv](on_hand.tsv) | full reload | Snapshot WMS, quotidien |
| purchase_orders | [purchase_orders.md](purchase_orders.md) — [purchase_orders.tsv](purchase_orders.tsv) | full reload | ERP, refresh quotidien |
| work_orders | [work_orders.md](work_orders.md) — [work_orders.tsv](work_orders.tsv) | full reload | MES, refresh quotidien |
| customer_orders | [customer_orders.md](customer_orders.md) — [customer_orders.tsv](customer_orders.tsv) | full reload | ERP commercial |
| transfers | [transfers.md](transfers.md) — [transfers.tsv](transfers.tsv) | full reload | Inter-locations |
| forecasts | [forecasts.md](forecasts.md) — [forecasts.tsv](forecasts.tsv) | full reload | Outil prévision |

`item_planning_params` reste à faire (modèle SCD2 plus complexe) — n'est pas dans la CHECK constraint actuelle de `ingest_batches.entity_type` ; sera ajouté quand le pipeline SCD2 sera câblé.
